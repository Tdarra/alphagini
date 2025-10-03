import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: 'standalone',     // smaller Cloud Run image
  reactStrictMode: true
};

export default nextConfig;
