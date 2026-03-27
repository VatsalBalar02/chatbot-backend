export function handleResponseType(responseType, projected, question) {
  switch (responseType) {

    case "YES_NO":
      if (projected.length === 1) {
        const c = projected[0];
        if (question.toLowerCase().includes("studying")) {
           const isStudying = c.Profile?.isStudying;


          return {
            success: true,
           answer: isStudying
              ? `Yes, ${c.FullName} is currently studying.`
              : `No, ${c.FullName} is not currently studying.`,
            dataframe: projected,
          };
        }
      }
      return null;

    // case "COUNT":
    //   return {
    //     success: true,
    //     answer: `There are ${projected.length} candidates matching your request.`,
    //     dataframe: projected,
    //   };

    case "LIST":
      const names = projected.map(c => c.FullName || "unknown");
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
  const MAX_NAMES = 20; // safety cap
  const finalList = projected.slice(0, MAX_NAMES);

  const names = finalList.map((c, i) => `${i + 1}. ${c.FullName}`);

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
    q.startsWith("does ") ||
    q.startsWith("do ") ||
    q.startsWith("has ") ||
    q.startsWith("have ");

  if (isYesNoQuestion && isSingle) {
    return "YES_NO";
  }

  // 🔥 2. Prevent NAME_LIST override when multiple sections requested
  if (responseType === "NAME_LIST" && contentSections >= 1) {
    if (q.includes("skill") || q.includes("education") || q.includes("experience")) {
      return "MULTI_SECTION";
    }
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
