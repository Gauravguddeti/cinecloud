import { create } from "zustand";
import type { User, Recommendation, Rating, Movie } from "@/lib/types";
import { authApi, ratingsApi, recommendationsApi } from "@/lib/api";
import { signInWithEmailAndPassword, signOut } from "firebase/auth";
import { firebaseAuth } from "@/lib/firebase";

interface AppState {
  // Auth
  user: User | null;
  accessToken: string | null;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string, name: string) => Promise<void>;
  logout: () => void;
  loadUser: () => Promise<void>;

  // Ratings
  ratings: Record<string, number>;         // movieId → star rating
  submitRating: (movieId: string, rating: number) => Promise<void>;
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
  accessToken: null,
  isAuthenticated: false,

  login: async (email, password) => {
    // Sign in via Firebase SDK — handles token issuance, refresh, etc.
    const credential = await signInWithEmailAndPassword(firebaseAuth, email, password);
    const idToken = await credential.user.getIdToken();
    localStorage.setItem("accessToken", idToken);
    const { data } = await authApi.getProfile();
    set({ user: data.user, accessToken: idToken, isAuthenticated: true });
    await Promise.all([get().loadRatings(), get().loadRecommendations()]);
  },

  register: async (email, password, name) => {
    await authApi.register(email, password, name);
    // Auto-login after register
    await get().login(email, password);
  },

  logout: () => {
    signOut(firebaseAuth).catch(() => {});
    localStorage.removeItem("accessToken");
    set({ user: null, accessToken: null, isAuthenticated: false, ratings: {}, recommendations: [] });
  },

  loadUser: async () => {
    const fbUser = firebaseAuth.currentUser;
    if (!fbUser) return;
    try {
      const idToken = await fbUser.getIdToken();
      localStorage.setItem("accessToken", idToken);
      const { data } = await authApi.getProfile();
      set({ user: data.user, accessToken: idToken, isAuthenticated: true });
    } catch {
      get().logout();
    }
  },

  // ── Ratings ───────────────────────────────────────────────
  ratings: {},

  submitRating: async (movieId, rating) => {
    const { user } = get();
    if (!user) return;
    await ratingsApi.submit(movieId, rating);
    set((state) => ({ ratings: { ...state.ratings, [movieId]: rating } }));
    // Recommendations are updated automatically via Firestore onSnapshot in useRealtimeRecs
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
