import { getAllLinks } from "./getAllLinks.js";

const url = process.argv[2];
if (!url) {
  console.error("Usage: node index.js <url>");
  process.exit(1);
}
if (!/^https?:\/\//i.test(url.trim())) {
  console.error("URL must start with http:// or https://");
  process.exit(1);
}

const links = await getAllLinks(url);

for (const link of links) {
  console.log(link);
}
