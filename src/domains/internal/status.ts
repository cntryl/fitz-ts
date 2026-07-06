export function formatStatusName(status: number, names: Record<number, string>): string {
  return names[status] ?? `Unknown(${status})`;
}
