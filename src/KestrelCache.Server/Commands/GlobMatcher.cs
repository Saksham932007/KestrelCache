namespace KestrelCache.Server.Commands;

/// <summary>
/// The glob matching Redis's <c>KEYS</c> and <c>SCAN MATCH</c> use: <c>*</c>, <c>?</c>,
/// character classes, and <c>\</c> escapes.
/// </summary>
/// <remarks>
/// Implemented directly rather than translated to a regular expression, for two reasons. The
/// semantics differ in small ways that would have to be special-cased anyway, and translating
/// user input into a regex invites catastrophic backtracking — a pattern like <c>(a+)+b</c> has
/// no glob equivalent, but a careless translation can manufacture one. The iterative matcher
/// below runs in time proportional to the product of the lengths and cannot blow up.
/// </remarks>
internal static class GlobMatcher
{
    /// <summary>Whether <paramref name="value"/> matches <paramref name="pattern"/>.</summary>
    internal static bool Matches(string pattern, string value)
    {
        int patternIndex = 0;
        int valueIndex = 0;
        int starPattern = -1;
        int starValue = 0;

        while (valueIndex < value.Length)
        {
            if (patternIndex < pattern.Length)
            {
                char token = pattern[patternIndex];

                switch (token)
                {
                    case '*':
                        // Remember where the star was so a failed match can backtrack to it and
                        // let the star swallow one more character. This is what keeps the
                        // matcher iterative instead of recursive.
                        starPattern = patternIndex++;
                        starValue = valueIndex;
                        continue;

                    case '?':
                        patternIndex++;
                        valueIndex++;
                        continue;

                    case '[':
                    {
                        if (TryMatchClass(pattern, ref patternIndex, value[valueIndex]))
                        {
                            valueIndex++;
                            continue;
                        }
                        break;
                    }

                    case '\\' when patternIndex + 1 < pattern.Length:
                        if (pattern[patternIndex + 1] == value[valueIndex])
                        {
                            patternIndex += 2;
                            valueIndex++;
                            continue;
                        }
                        break;

                    default:
                        if (token == value[valueIndex])
                        {
                            patternIndex++;
                            valueIndex++;
                            continue;
                        }
                        break;
                }
            }

            if (starPattern < 0) return false;

            patternIndex = starPattern + 1;
            valueIndex = ++starValue;
        }

        // Trailing stars may match the empty remainder.
        while (patternIndex < pattern.Length && pattern[patternIndex] == '*')
        {
            patternIndex++;
        }

        return patternIndex == pattern.Length;
    }

    /// <summary>
    /// Matches a <c>[...]</c> class, advancing <paramref name="patternIndex"/> past it either way.
    /// </summary>
    private static bool TryMatchClass(string pattern, ref int patternIndex, char candidate)
    {
        int index = patternIndex + 1;
        bool negated = index < pattern.Length && pattern[index] == '^';
        if (negated) index++;

        bool matched = false;

        while (index < pattern.Length && pattern[index] != ']')
        {
            if (pattern[index] == '\\' && index + 1 < pattern.Length)
            {
                index++;
                if (pattern[index] == candidate) matched = true;
            }
            else if (index + 2 < pattern.Length
                && pattern[index + 1] == '-'
                && pattern[index + 2] != ']')
            {
                char low = pattern[index];
                char high = pattern[index + 2];
                if (low > high) (low, high) = (high, low);
                if (candidate >= low && candidate <= high) matched = true;
                index += 2;
            }
            else if (pattern[index] == candidate)
            {
                matched = true;
            }

            index++;
        }

        // Advance past the closing bracket, or to the end if the class was unterminated.
        patternIndex = index < pattern.Length ? index + 1 : index;
        return negated ? !matched : matched;
    }

    /// <summary>
    /// The longest literal prefix of a pattern, so a glob can be narrowed to a range scan.
    /// </summary>
    /// <remarks>
    /// This is what makes <c>KEYS user:*</c> cost the <c>user:</c> range rather than the whole
    /// database. It stops at the first metacharacter, because everything after that could match
    /// anything.
    /// </remarks>
    internal static string LiteralPrefixOf(string pattern)
    {
        int index = 0;
        var prefix = new System.Text.StringBuilder();

        while (index < pattern.Length)
        {
            char token = pattern[index];

            if (token is '*' or '?' or '[') break;

            if (token == '\\' && index + 1 < pattern.Length)
            {
                prefix.Append(pattern[index + 1]);
                index += 2;
                continue;
            }

            prefix.Append(token);
            index++;
        }

        return prefix.ToString();
    }
}
