// SSR uses internal URL, client uses NEXT_PUBLIC_API_URL
const API_BASE_URL = (() => {
  // Server-side: use internal URL for direct connection
  if (typeof window === "undefined") {
    return process.env.API_URL_INTERNAL
      ? `${process.env.API_URL_INTERNAL}/api`
      : "http://localhost:8000/api";
  }
  // Client-side: use public URL (can be /api proxy or full URL)
  return process.env.NEXT_PUBLIC_API_URL || "/api";
})();

interface RequestOptions extends RequestInit {
  params?: Record<string, string | number | boolean | undefined | null>;
}

class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  private buildUrl(
    path: string,
    params?: Record<string, string | number | boolean | undefined | null>
  ): string {
    const fullPath = `${this.baseUrl}${path}`;
    // Support relative URLs (e.g. /api/...) in client-side context
    const base = typeof window !== "undefined" ? window.location.origin : "http://localhost:3000";
    const url = new URL(fullPath, base);
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
          url.searchParams.set(key, String(value));
        }
      });
    }
    return url.toString();
  }

  async get<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const { params, ...fetchOptions } = options;
    const url = this.buildUrl(path, params);
    const response = await fetch(url, {
      ...fetchOptions,
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        ...fetchOptions.headers,
      },
    });

    if (!response.ok) {
      const error = await response.json().catch(() => null);
      throw new ApiError(
        response.status,
        error?.error?.message || response.statusText
      );
    }

    return response.json();
  }

  async patch<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const { params, ...fetchOptions } = options;
    const url = this.buildUrl(path, params);
    const response = await fetch(url, {
      ...fetchOptions,
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        ...fetchOptions.headers,
      },
    });

    if (!response.ok) {
      const error = await response.json().catch(() => null);
      throw new ApiError(
        response.status,
        error?.error?.message || response.statusText
      );
    }

    return response.json();
  }
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export const api = new ApiClient(API_BASE_URL);
