import axios from "axios";
import type { LoginResponse, Movie, Recommendation, Rating } from "./types";

// Single Render.com backend URL — set in frontend/.env.local
const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

const api = axios.create({ baseURL: API_URL });

// AppProvider registers Clerk's getToken here so interceptor always gets a fresh JWT
let _getToken: (() => Promise<string | null>) | null = null;
export function setTokenProvider(fn: () => Promise<string | null>) {
  _getToken = fn;
}

// Attach fresh Clerk JWT on every request (avoids short-lived JWT expiry issues)
api.interceptors.request.use(async (config) => {
  if (typeof window !== "undefined") {
    let token: string | null = null;
    if (_getToken) {
      token = await _getToken();
      if (token) localStorage.setItem("accessToken", token);
    } else {
      token = localStorage.getItem("accessToken");
    }
    if (token) config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// On 401: clear cached token — Clerk SDK will handle session state, no hard redirect
api.interceptors.response.use(
  (res) => res,
  (error) => {
    if (error.response?.status === 401) {
      if (typeof window !== "undefined") {
        localStorage.removeItem("accessToken");
      }
    }
    return Promise.reject(error);
  }
);

// ── Auth ──────────────────────────────────────────────────────
export const authApi = {
  /** Called after Clerk sign-in to upsert the user row in NeonDB */
  sync: (email: string, name: string) =>
    api.post<{ user: import("./types").User }>("/auth/sync", { email, name }),

  getProfile: () => api.get<{ user: import("./types").User }>("/auth/profile"),
};

// ── Movies ────────────────────────────────────────────────────
export const moviesApi = {
  list: (params?: { genre?: string; limit?: number; nextToken?: string }) =>
    api.get<{ movies: Movie[]; count: number; nextToken?: string }>("/movies/list", { params }),

  get: (movieId: string) =>
    api.get<{ movie: Movie }>(`/movies/detail/${movieId}`),

  search: (q: string, signal?: AbortSignal) =>
    api.get<{ movies: Movie[]; count: number }>("/movies/search", { params: { q }, signal }),

  popular: (limit = 20) =>
    api.get<{ movies: Movie[]; count: number }>("/movies/popular", { params: { limit } }),

  genres: () =>
    api.get<{ genres: string[] }>("/movies/genres"),
};

// ── Ratings ───────────────────────────────────────────────────
export const ratingsApi = {
  submit: (movieId: string, rating: number) =>
    api.post<{ message: string; rating: Rating }>("/ratings/submit", { movieId, rating }),

  getUserRatings: (userId: string) =>
    api.get<{ ratings: Rating[]; count: number }>(`/ratings/user/${userId}`),

  deleteRating: (movieId: string) =>
    api.delete<{ message: string }>(`/ratings/delete/${movieId}`),

  resetRatings: () =>
    api.delete<{ message: string }>("/ratings/reset"),
};

// ── Recommendations ───────────────────────────────────────────
export const recommendationsApi = {
  get: (userId: string) =>
    api.get<{
      recommendations: Recommendation[];
      userId: string;
      fromCache: boolean;
      computeTimeSeconds?: number;
      count: number;
    }>(`/recommendations/${userId}`),

  refresh: (userId: string) =>
    api.post<{ recommendations: Recommendation[]; count: number }>(`/recommendations/${userId}/refresh`),
};

// ── Event Tracking ────────────────────────────────────────────
export const eventsApi = {
  track: (eventType: string, properties?: Record<string, unknown>) =>
    api.post("/events/track", { eventType, properties }).catch(() => {/* non-critical */}),
};

export default api;

