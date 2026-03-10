"use client";

import { useState } from "react";
import { useStore } from "@/lib/store";
import toast from "react-hot-toast";
import clsx from "clsx";

interface StarRatingProps {
  movieId: string;
  size?: "sm" | "md" | "lg";
}

export function StarRating({ movieId, size = "md" }: StarRatingProps) {
  const { ratings, submitRating, isAuthenticated } = useStore();
  const currentRating = ratings[movieId] || 0;
  const [hovered, setHovered] = useState(0);
  const [submitting, setSubmitting] = useState(false);

  const sizeClass = { sm: "text-lg", md: "text-2xl", lg: "text-3xl" }[size];

  const handleRate = async (star: number) => {
    if (!isAuthenticated) {
      toast.error("Sign in to rate movies");
      return;
    }
    if (submitting) return;
    setSubmitting(true);
    try {
      await submitRating(movieId, star);
      toast.success(`Rated ${star} ★ — updating your recommendations...`, {
        duration: 3000,
        style: { background: "#1a1a1a", color: "#fff", border: "1px solid #22c55e" },
      });
    } catch {
      toast.error("Failed to submit rating");
    }
    setSubmitting(false);
  };

  const display = hovered || currentRating;

  return (
    <div
      className="flex items-center gap-1"
      onMouseLeave={() => setHovered(0)}
      aria-label="Rate this movie"
    >
      {[1, 2, 3, 4, 5].map((star) => (
        <button
          key={star}
          onClick={() => handleRate(star)}
          onMouseEnter={() => setHovered(star)}
          disabled={submitting}
          className={clsx(
            sizeClass,
            "transition-all duration-100 hover:scale-125 disabled:opacity-50 cursor-pointer",
            star <= display ? "text-yellow-400" : "text-gray-600"
          )}
          aria-label={`Rate ${star} stars`}
        >
          ★
        </button>
      ))}
      {currentRating > 0 && (
        <span className="text-xs text-gray-400 ml-1">Your rating: {currentRating}/5</span>
      )}
    </div>
  );
}
