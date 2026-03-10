"use client";

import { useEffect } from "react";
import { useUser, useAuth } from "@clerk/nextjs";
import { useStore } from "@/lib/store";
import { useRealtimeRecs } from "@/hooks/useRealtimeRecs";
import { MovieDetailModal } from "@/components/MovieDetailModal";
import { authApi, setTokenProvider } from "@/lib/api";

/**
 * AppProvider — syncs Clerk auth state to store + NeonDB.
 * After every Clerk sign-in, calls POST /auth/sync to upsert the user row
 * and stores the Clerk JWT in localStorage for the Axios interceptor.
 */
export function AppProvider({ children }: { children: React.ReactNode }) {
  const { user: clerkUser, isSignedIn, isLoaded } = useUser();
  const { getToken } = useAuth();
  const { setUser, user, selectedMovie, setSelectedMovie, loadRatings, loadRecommendations } = useStore();

  // Register Clerk's getToken so every axios request gets a fresh JWT (prevents expiry redirects)
  useEffect(() => {
    setTokenProvider(getToken);
  }, [getToken]);

  useEffect(() => {
    if (!isLoaded) return;

    if (!isSignedIn || !clerkUser) {
      setUser(null);
      localStorage.removeItem("accessToken");
      return;
    }

    (async () => {
      try {
        const token = await getToken();
        if (!token) return;
        localStorage.setItem("accessToken", token);

        const email = clerkUser.primaryEmailAddress?.emailAddress ?? "";
        const name  = clerkUser.fullName ?? clerkUser.firstName ?? "";
        const { data } = await authApi.sync(email, name);
        setUser(data.user);
      } catch (err) {
        console.error("[AppProvider] sync failed:", err);
        setUser(null);
      }
    })();
  }, [isLoaded, isSignedIn, clerkUser?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load data once user is available in store
  useEffect(() => {
    if (user) {
      loadRatings();
      loadRecommendations();
    }
  }, [user?.userId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Poll for recommendation updates every 30 s
  useRealtimeRecs(user?.userId);

  return (
    <>
      {children}
      {selectedMovie && (
        <MovieDetailModal movie={selectedMovie} onClose={() => setSelectedMovie(null)} />
      )}
    </>
  );
}

