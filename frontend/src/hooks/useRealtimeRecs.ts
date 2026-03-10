/**
 * useRealtimeRecs — Firestore onSnapshot hook
 * ============================================
 * Replaces: AWS API Gateway WebSocket + DynamoDB connections table + Lambda authorizer
 *
 * How it works:
 *   1. User rates a movie → backend writes to Firestore "ratings" + publishes to Pub/Sub
 *   2. Cloud Pub/Sub triggers the recommendation worker Cloud Function
 *   3. Worker recomputes recommendations and writes to Firestore "recommendations/{userId}"
 *   4. THIS HOOK receives the update instantly via Firestore's real-time sync ⚡
 *
 * Zero extra infrastructure needed — Firestore handles all WebSocket-equivalent delivery.
 */
"use client";
import { useEffect, useRef } from "react";
import { doc, onSnapshot, Unsubscribe } from "firebase/firestore";
import toast from "react-hot-toast";
import { db } from "@/lib/firebase";
import { useStore } from "@/lib/store";

export function useRealtimeRecs(userId: string | null | undefined) {
  const setRecommendations = useStore((s) => s.setRecommendations);
  const unsubRef = useRef<Unsubscribe | null>(null);

  useEffect(() => {
    // Unsubscribe any previous listener
    if (unsubRef.current) {
      unsubRef.current();
      unsubRef.current = null;
    }

    if (!userId) return;

    console.log(`[Firestore] Subscribing to recommendations/${userId}`);

    unsubRef.current = onSnapshot(
      doc(db, "recommendations", userId),
      (snapshot) => {
        if (!snapshot.exists()) return;

        const data = snapshot.data();
        const recs = data?.recommendations ?? [];

        if (recs.length > 0) {
          setRecommendations(recs);
          toast("✨ Recommendations updated in real-time!", {
            icon: "🎬",
            duration: 2500,
          });
          console.log(`[Firestore] Received ${recs.length} updated recommendations in real-time`);
        }
      },
      (error) => {
        console.warn("[Firestore] onSnapshot error:", error.message);
      }
    );

    return () => {
      if (unsubRef.current) {
        unsubRef.current();
        unsubRef.current = null;
        console.log(`[Firestore] Unsubscribed from recommendations/${userId}`);
      }
    };
  }, [userId, setRecommendations]);
}
