"use client";

import Image from "next/image";
import { useRef } from "react";
import { useStore } from "@/lib/store";
import { eventsApi } from "@/lib/api";
import type { Movie } from "@/lib/types";
import { StarRating } from "./StarRating";
import clsx from "clsx";

interface MovieCardProps {
  movie: Movie;
  showRating?: boolean;
  reason?: string;
  className?: string;
}

export function MovieCard({ movie, showRating = true, reason, className }: MovieCardProps) {
  const { setSelectedMovie, ratings } = useStore();
  const userRating = ratings[movie.movieId];
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleMouseEnter = () => {
    hoverTimer.current = setTimeout(() => {
      eventsApi.track("browse_hover", { movieId: movie.movieId, title: movie.title });
    }, 2000);
  };

  const handleMouseLeave = () => {
    if (hoverTimer.current) clearTimeout(hoverTimer.current);
  };

  return (
    <div
      className={clsx("movie-card group", className)}
      onClick={() => setSelectedMovie(movie)}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === "Enter" && setSelectedMovie(movie)}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      aria-label={`View ${movie.title}`}
    >
      {/* Poster */}
      <div className="aspect-[2/3] bg-brand-card rounded-lg overflow-hidden">
        {movie.posterPath ? (
          <Image
            src={movie.posterPath}
            alt={movie.title}
            width={300}
            height={450}
            className="w-full h-full object-cover"
            unoptimized
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center bg-brand-card">
            <span className="text-4xl">🎬</span>
          </div>
        )}
        {/* Hover overlay */}
        <div className="poster-overlay flex flex-col justify-end p-3">
          <p className="text-xs text-white/80 line-clamp-2">{movie.overview}</p>
          {reason && (
            <p className="text-xs text-brand-red mt-1 font-medium line-clamp-1">{reason}</p>
          )}
        </div>
      </div>

      {/* Info bar */}
      <div className="mt-2 px-0.5">
        <h3 className="text-sm font-semibold line-clamp-1">{movie.title}</h3>
        <div className="flex items-center justify-between mt-1">
          <span className="text-xs text-gray-400">{movie.releaseYear}</span>
          <div className="flex items-center gap-1">
            <span className="text-yellow-400 text-xs">★</span>
            <span className="text-xs text-gray-300">{Number(movie.voteAverage).toFixed(1)}</span>
          </div>
        </div>
        {showRating && userRating && (
          <div className="flex gap-0.5 mt-1">
            {[1, 2, 3, 4, 5].map((s) => (
              <span key={s} className={clsx("text-xs", s <= userRating ? "text-yellow-400" : "text-gray-600")}>★</span>
            ))}
          </div>
        )}
        {movie.genres?.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-1">
            {movie.genres.slice(0, 2).map((g) => (
              <span key={g} className="text-xs bg-white/10 px-1.5 py-0.5 rounded text-gray-300">{g}</span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
