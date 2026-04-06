export function handleResponseType(
  responseType,
  projected,
  question,
  analysis,
) {
  const q = question.toLowerCase();
  switch (responseType) {
    case "YES_NO": {
      if (projected.length !== 1) return null;

      const c = projected[0];
      const YES_NO_FIELDS = {
        studying: () => c.Profile?.isStudying,
        verified: () => c.isVerified,
        shortlisted: () => c.Applications?.some((a) => a.isShortlisted),
      };

      for (const key in YES_NO_FIELDS) {
        if (q.includes(key)) {
          const value = YES_NO_FIELDS[key]();

          if (value == null) {
            return {
              success: true,
              answer: `I don't have enough data to determine that for ${c.FullName}.`,
              dataframe: projected,
            };
          }
          const readableKey =
            {
              studying: "currently studying",
              verified: "verified",
              shortlisted: "shortlisted",
            }[key] || key;

          return {
            success: true,
            answer: value
              ? `Yes, ${c.FullName} is ${readableKey}.`
              : `No, ${c.FullName} is not ${readableKey}.`,
            dataframe: projected,
          };
        }
      }
      return {
        success: true,
        answer: `I couldn't determine that from available data for ${c.FullName}.`,
        dataframe: projected,
      };
    }

    case "COUNT":
      return {
        success: true,
        answer: `There are ${projected.length} candidates matching your request.`,
        dataframe: projected,
      };

    case "LIST":
      const names = projected.map((c) => {
        const city = c.Profile?.city ? ` (${c.Profile.city})` : "";
        return (c.FullName || "unknown") + city;
      });
      return {
        success: true,
        answer: names.length
          ? `Here are the candidates:\n\n- ${names.join("\n- ")}`
          : "No candidates found.",
        dataframe: projected,
      };

    case "NAME_LIST": {
      if (!projected || projected.length === 0) {
        return {
          success: true,
          answer: "No candidates found.",
          dataframe: [],
        };
      }

      // 🔥 Respect user limit (if already sliced, safe)
      const MAX_NAMES = analysis?.limit ? Math.min(analysis.limit, 50) : 50;
      const finalList = projected.slice(0, MAX_NAMES);

      const names = finalList.map(
        (c, i) => `${i + 1}. ${c.FullName || "Unknown"}`,
      );

      let answer = `Here are the candidate names:\n\n${names.join("\n")}`;

      if (projected.length > MAX_NAMES) {
        answer += `\n\n...and ${projected.length - MAX_NAMES} more candidates.`;
      }

      return {
        success: true,
        answer,
        dataframe: finalList,
      };
    }

    case "WITH_SKILLS":
    case "WITH_EDUCATION":
    case "WITH_PROFILE":
    case "WITH_EXPERIENCE":
    case "WITH_DOCUMENTS":
    case "WITH_RESUMES":
    case "WITH_APPLICATIONS":
    case "MULTI_SECTION":
      return null; // fallback to LLM

    default:
      return null;
  }
}

export function refineResponseType({
  responseType,
  question,
  projected,
  analysis,
  contentSections,
}) {
  const q = question.toLowerCase();

  const isSingle = projected.length === 1;

  // 🔥 1. YES/NO detection override
  const isYesNoQuestion =
    q.startsWith("is ") ||
    q.startsWith("are ") ||
    q.startsWith("was ") ||
    q.startsWith("were ") ||
    q.startsWith("does ") ||
    q.startsWith("do ") ||
    q.startsWith("did ") ||
    q.startsWith("has ") ||
    q.startsWith("have ") ||
    q.startsWith("can ");

  if (isYesNoQuestion && isSingle) {
    return "YES_NO";
  }

  // 🔥 2. Prevent NAME_LIST override when multiple sections requested
  if (responseType === "NAME_LIST" && !isSingle) {
    return "NAME_LIST";
  }

  // 🔥 3. Single candidate + section → specific type
  if (isSingle) {
    if (q.includes("skill")) return "WITH_SKILLS";
    if (q.includes("education")) return "WITH_EDUCATION";
    if (q.includes("experience")) return "WITH_EXPERIENCE";
    if (q.includes("profile")) return "WITH_PROFILE";
  }

  // 🔥 4. Prevent LIST for single candidate
  if (responseType === "LIST" && isSingle) {
    return "WITH_PROFILE";
  }

  return responseType;
}
