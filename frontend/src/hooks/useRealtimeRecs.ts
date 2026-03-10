/**
 * useRealtimeRecs — polls /recommendations/<userId> every 30 s
 * =============================================================
 * Replaces: Firestore onSnapshot (which required Firebase SDK)
 *
 * After a user rates a movie the backend recomputes recs in a background
 * thread and writes them to the recommendations table.  This hook polls
 * for fresh results so the UI stays up-to-date without WebSockets or
 * Firestore real-time listeners.
 */
"use client";
import { useEffect, useRef } from "react";
import toast from "react-hot-toast";
import { recommendationsApi } from "@/lib/api";
import { useStore } from "@/lib/store";

const POLL_INTERVAL_MS = 30_000; // 30 s

export function useRealtimeRecs(userId: string | null | undefined) {
  const setRecommendations = useStore((s) => s.setRecommendations);
  const prevCountRef       = useRef<number>(-1);

  useEffect(() => {
    if (!userId) return;

    const poll = async () => {
      try {
        const { data } = await recommendationsApi.get(userId);
        const recs = data.recommendations ?? [];
        if (recs.length > 0 && recs.length !== prevCountRef.current && prevCountRef.current !== -1) {
          setRecommendations(recs);
          toast("✨ Recommendations updated!", { icon: "🎬", duration: 2500 });
          console.log(`[recs] Polled ${recs.length} recommendations`);
        } else if (recs.length > 0) {
          setRecommendations(recs);
        }
        prevCountRef.current = recs.length;
      } catch (e) {
        console.warn("[recs] Poll error:", e);
      }
    };

    poll();
    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [userId]); // eslint-disable-line react-hooks/exhaustive-deps
}

