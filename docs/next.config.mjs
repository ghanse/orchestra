import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

const repoBasePath = process.env.DOCS_BASE_PATH ?? '/orchestra';

/** @type {import('next').NextConfig} */
const config = {
  output: 'export',
  reactStrictMode: true,
  basePath: repoBasePath,
  trailingSlash: true,
  images: {
    unoptimized: true,
  },
};

export default withMDX(config);
