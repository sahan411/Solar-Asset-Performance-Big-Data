// Serializes an already-fetched API response to CSV. Never recomputes a
// figure — every value here is copied verbatim from the report the user is
// already looking at, so the export can never disagree with the screen.

export function toCsv(headers: string[], rows: (string | number | boolean | null)[][]): string {
  const escape = (value: string | number | boolean | null): string => {
    if (value === null || value === undefined) return "";
    const text = String(value);
    return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };

  const lines = [headers.map(escape).join(","), ...rows.map((row) => row.map(escape).join(","))];
  return lines.join("\n");
}

export function downloadCsv(filename: string, csv: string): void {
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}
