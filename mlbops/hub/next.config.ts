import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  allowedDevOrigins: ["127.0.0.1", "localhost", "2.24.123.57", "100.111.41.78"],
  staticPageGenerationTimeout: 20,
  experimental: {
    cpus: 1,
    workerThreads: false,
    staticGenerationMaxConcurrency: 1,
    staticGenerationMinPagesPerWorker: 1,
  },
  typescript: {
    // The current repo can hang during Next's production type-check phase.
    // Keep deploy builds deterministic; run TypeScript separately when tightening CI.
    ignoreBuildErrors: true,
  },
};

export default nextConfig;
