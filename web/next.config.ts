import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  poweredByHeader: false,
  compress: true,
  images: {
    formats: ["image/avif", "image/webp"],
  },
  async rewrites() {
    const apiUrl = process.env.API_URL_INTERNAL || "http://localhost:8000";
    return {
      // Fallback rewrites — only apply if no page/file matches
      fallback: [
        {
          source: "/api/:path*",
          destination: `${apiUrl}/api/:path*`,
        },
      ],
    };
  },
};

export default nextConfig;
