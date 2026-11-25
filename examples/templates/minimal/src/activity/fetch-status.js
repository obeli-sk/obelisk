export default async function fetchStatus() {
  const response = await fetch("https://example.com/");
  return response.status;
}
