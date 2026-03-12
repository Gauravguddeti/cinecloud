import { create } from "zustand";
import type { User, Recommendation, Rating, Movie } from "@/lib/types";
import { ratingsApi, recommendationsApi } from "@/lib/api";

interface AppState {
  // Auth (managed by AppProvider via Clerk — set after /auth/sync)
  user: User | null;
  isAuthenticated: boolean;
  setUser: (user: User | null) => void;

  // Ratings
  ratings: Record<string, number>;         // movieId → star rating
  submitRating: (movieId: string, rating: number) => Promise<void>;
  deleteRating: (movieId: string) => Promise<void>;
  resetRatings: () => Promise<void>;
  loadRatings: () => Promise<void>;

  // Recommendations
  recommendations: Recommendation[];
  recsLoading: boolean;
  recsFromCache: boolean;
  loadRecommendations: () => Promise<void>;
  setRecommendations: (recs: Recommendation[]) => void;

  // Selected movie (detail modal)
  selectedMovie: Movie | null;
  setSelectedMovie: (movie: Movie | null) => void;
}

export const useStore = create<AppState>((set, get) => ({
  // ── Auth ─────────────────────────────────────────────────
  user: null,
  isAuthenticated: false,

  setUser: (user) => set({ user, isAuthenticated: !!user }),

  // ── Ratings ───────────────────────────────────────────────
  ratings: {},

  submitRating: async (movieId, rating) => {
    const { user } = get();
    if (!user) return;
    await ratingsApi.submit(movieId, rating);
    set((state) => ({ ratings: { ...state.ratings, [movieId]: rating } }));
    // Background thread on server takes ~2-3 s to recompute; reload after that.
    setTimeout(() => get().loadRecommendations(), 3500);
  },

  deleteRating: async (movieId) => {
    const { user } = get();
    if (!user) return;
    await ratingsApi.deleteRating(movieId);
    set((state) => {
      const next = { ...state.ratings };
      delete next[movieId];
      return { ratings: next };
    });
  },

  resetRatings: async () => {
    const { user } = get();
    if (!user) return;
    await ratingsApi.resetRatings();
    set({ ratings: {}, recommendations: [] });
    // Force-clear Redis cache and pull fresh recs immediately.
    // Without this, old cached recommendations survive for up to 30 min.
    try {
      const { data } = await recommendationsApi.refresh(user.userId);
      set({ recommendations: data.recommendations ?? [], recsFromCache: false });
    } catch {}
  },

  loadRatings: async () => {
    const { user } = get();
    if (!user) return;
    try {
      const { data } = await ratingsApi.getUserRatings(user.userId);
      const ratingsMap: Record<string, number> = {};
      data.ratings.forEach((r) => { ratingsMap[r.movieId] = r.rating; });
      set({ ratings: ratingsMap });
    } catch {}
  },

  // ── Recommendations ───────────────────────────────────────
  recommendations: [],
  recsLoading: false,
  recsFromCache: false,

  loadRecommendations: async () => {
    const { user } = get();
    if (!user) return;
    set({ recsLoading: true });
    try {
      const { data } = await recommendationsApi.get(user.userId);
      set({
        recommendations: data.recommendations,
        recsLoading: false,
        recsFromCache: data.fromCache,
      });
    } catch {
      set({ recsLoading: false });
    }
  },

  setRecommendations: (recs) => set({ recommendations: recs }),

  // ── Selected Movie ────────────────────────────────────────
  selectedMovie: null,
  setSelectedMovie: (movie) => set({ selectedMovie: movie }),
}));
