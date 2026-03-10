"use client";

import { useEffect } from "react";
import { onAuthStateChanged } from "firebase/auth";
import { useStore } from "@/lib/store";
import { useRealtimeRecs } from "@/hooks/useRealtimeRecs";
import { MovieDetailModal } from "@/components/MovieDetailModal";
import { firebaseAuth } from "@/lib/firebase";

/**
 * AppProvider — wraps the app with global state initialization.
 * Uses Firestore onSnapshot for real-time updates (replaces WebSocket).
 */
export function AppProvider({ children }: { children: React.ReactNode }) {
  const { loadUser, user, selectedMovie, setSelectedMovie } = useStore();

  // Sync Firebase Auth state on mount
  useEffect(() => {
    const unsub = onAuthStateChanged(firebaseAuth, (fbUser) => {
      if (fbUser) {
        loadUser();
      }
    });
    return () => unsub();
  }, [loadUser]);

  // Subscribe to real-time recommendation updates via Firestore onSnapshot ⚡
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
