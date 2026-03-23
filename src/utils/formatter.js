export function cleanForPdf(text) {
  return (text || "").replace(/[^\x00-\x7F]/g, "").trim();
}

export function formatChatAnswer(result) {
  const { type, answer, dataframe } = result;

  if (type === "OUT_OF_SCOPE" || type === "PDF") return answer;

  if (type === "REPORT") return answer;

  if (type === "SQL" && dataframe && dataframe.length > 0) {
    const count = dataframe.length;
    const cols = Object.keys(dataframe[0]);

    const explanation = answer.includes("\n\n")
      ? answer.split("\n\n").slice(1).join("\n\n")
      : "";

    let summary = `Found **${count} record${count !== 1 ? "s" : ""}**.`;
    if (explanation) summary += `\n\n${explanation}`;

    const displayRows = dataframe.slice(0, 20);
    const header = `| ${cols.join(" | ")} |`;
    const divider = `| ${cols.map(() => "---").join(" | ")} |`;
    const rows = displayRows.map(
      (row) => `| ${cols.map((c) => String(row[c] ?? "—")).join(" | ")} |`,
    );

    const table = [header, divider, ...rows].join("\n");

    let formatted = `${summary}\n\n${table}`;
    if (count > 20) {
      formatted += `\n\n_Showing 20 of ${count} records. Generate a report to see all._`;
    }
    return formatted;
  }

  if (type === "SQL" && (!dataframe || dataframe.length === 0)) {
    return "No records found matching your query.";
  }

  return answer;
}
