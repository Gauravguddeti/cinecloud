import axios from "axios";
import type { LoginResponse, Movie, Recommendation, Rating } from "./types";

// Single Render.com backend URL — set in frontend/.env.local
const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

const api = axios.create({ baseURL: API_URL });

// Attach Firebase ID token on every request
api.interceptors.request.use((config) => {
  if (typeof window !== "undefined") {
    const token = localStorage.getItem("accessToken");
    if (token) config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// On 401: ask Firebase SDK for a fresh token and retry once
api.interceptors.response.use(
  (res) => res,
  async (error) => {
    const original = error.config;
    if (error.response?.status === 401 && !original._retry) {
      original._retry = true;
      try {
        const { firebaseAuth } = await import("@/lib/firebase");
        const user = firebaseAuth.currentUser;
        if (user) {
          const freshToken = await user.getIdToken(true);
          localStorage.setItem("accessToken", freshToken);
          original.headers.Authorization = `Bearer ${freshToken}`;
          return api(original);
        }
      } catch {
        localStorage.removeItem("accessToken");
        if (typeof window !== "undefined") window.location.href = "/login";
      }
    }
    return Promise.reject(error);
  }
);

// ── Auth ──────────────────────────────────────────────────────
export const authApi = {
  register: (email: string, password: string, name: string) =>
    api.post<{ message: string; userId: string }>("/auth/register", { email, password, name }),

  login: (email: string, password: string) =>
    api.post<LoginResponse>("/auth/login", { email, password }),

  getProfile: () => api.get<{ user: import("./types").User }>("/auth/profile"),
};

// ── Movies ────────────────────────────────────────────────────
export const moviesApi = {
  list: (params?: { genre?: string; limit?: number; nextToken?: string }) =>
    api.get<{ movies: Movie[]; count: number; nextToken?: string }>("/movies/list", { params }),

  get: (movieId: string) =>
    api.get<{ movie: Movie }>(`/movies/detail/${movieId}`),

  search: (q: string) =>
    api.get<{ movies: Movie[]; count: number }>("/movies/search", { params: { q } }),

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

