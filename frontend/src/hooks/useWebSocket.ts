"use client";

import { useEffect, useRef, useCallback } from "react";
import { useStore } from "@/lib/store";
import type { WsMessage } from "@/lib/types";
import toast from "react-hot-toast";

const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "";
const RECONNECT_DELAY_MS = 3000;
const MAX_RECONNECT_ATTEMPTS = 5;

/**
 * useWebSocket
 * Establishes and maintains a persistent WebSocket connection for real-time
 * recommendation updates pushed from the recommendation worker Lambda.
 */
export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectCountRef = useRef(0);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const { isAuthenticated, setRecommendations } = useStore();

  const sendEvent = useCallback((type: string, payload: object = {}) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type, payload }));
    }
  }, []);

  const connect = useCallback(() => {
    if (!isAuthenticated || !WS_URL) return;

    const token = localStorage.getItem("accessToken");
    if (!token) return;

    // Close any existing connection
    wsRef.current?.close();

    const url = `${WS_URL}?token=${encodeURIComponent(token)}`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log("[ws] Connected to CineCloud real-time");
      reconnectCountRef.current = 0;
      // Send initial page view event
      sendEvent("PAGE_VIEW", { page: "home" });
    };

    ws.onmessage = (event) => {
      try {
        const message: WsMessage = JSON.parse(event.data);

        if (message.type === "RECOMMENDATIONS_UPDATED" && message.recommendations) {
          console.log("[ws] Received updated recommendations");
          setRecommendations(message.recommendations);
          toast("✨ Your recommendations just updated!", {
            duration: 3000,
            style: {
              background: "#1a1a1a",
              color: "#fff",
              border: "1px solid #E50914",
            },
          });
        }
      } catch (e) {
        console.error("[ws] Message parse error:", e);
      }
    };

    ws.onerror = (err) => {
      console.error("[ws] WebSocket error:", err);
    };

    ws.onclose = () => {
      console.log("[ws] Connection closed");
      // Auto-reconnect with backoff
      if (reconnectCountRef.current < MAX_RECONNECT_ATTEMPTS && isAuthenticated) {
        reconnectCountRef.current += 1;
        const delay = RECONNECT_DELAY_MS * reconnectCountRef.current;
        console.log(`[ws] Reconnecting in ${delay}ms (attempt ${reconnectCountRef.current})`);
        reconnectTimerRef.current = setTimeout(connect, delay);
      }
    };
  }, [isAuthenticated, sendEvent, setRecommendations]);

  useEffect(() => {
    if (isAuthenticated) {
      connect();
    } else {
      wsRef.current?.close();
    }

    return () => {
      clearTimeout(reconnectTimerRef.current ?? undefined);
      wsRef.current?.close();
    };
  }, [isAuthenticated, connect]);

  return { sendEvent };
}
