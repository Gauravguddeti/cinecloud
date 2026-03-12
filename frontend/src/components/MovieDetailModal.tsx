"use client";

import { useEffect } from "react";
import Image from "next/image";
import { motion, AnimatePresence } from "framer-motion";
import { useStore } from "@/lib/store";
import { StarRating } from "./StarRating";
import { eventsApi } from "@/lib/api";
import type { Movie } from "@/lib/types";

interface MovieDetailModalProps {
  movie: Movie;
  onClose: () => void;
}

export function MovieDetailModal({ movie, onClose }: MovieDetailModalProps) {
  // Track movie_view on mount
  useEffect(() => {
    eventsApi.track("movie_view", { movieId: movie.movieId, title: movie.title });
  }, [movie.movieId]);  // eslint-disable-line react-hooks/exhaustive-deps

  // Close on Escape key
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [onClose]);

  return (
    <AnimatePresence>
      <motion.div
        className="fixed inset-0 z-[100] flex items-center justify-center p-4"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
      >
        {/* Backdrop */}
        <div
          className="absolute inset-0 bg-black/80 backdrop-blur-sm"
          onClick={onClose}
          aria-hidden="true"
        />

        {/* Modal */}
        <motion.div
          className="relative bg-brand-card border border-brand-border rounded-2xl overflow-hidden max-w-2xl w-full max-h-[90vh] overflow-y-auto shadow-2xl"
          initial={{ scale: 0.9, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          exit={{ scale: 0.9, opacity: 0 }}
          transition={{ type: "spring", damping: 25, stiffness: 300 }}
        >
          {/* Backdrop image */}
          {movie.backdropPath || movie.posterPath ? (
            <div className="relative h-56 bg-gray-900">
              <Image
                src={movie.backdropPath || movie.posterPath!}
                alt={movie.title}
                fill
                className="object-cover opacity-60"
                unoptimized
              />
              <div className="absolute inset-0 bg-gradient-to-t from-brand-card to-transparent" />
            </div>
          ) : (
            <div className="h-24 bg-gradient-to-r from-brand-red/20 to-purple-900/20" />
          )}

          {/* Close button */}
          <button
            onClick={onClose}
            className="absolute top-4 right-4 w-8 h-8 rounded-full bg-black/60 flex items-center justify-center hover:bg-black transition-colors z-10"
            aria-label="Close"
          >
            ✕
          </button>

          {/* Content */}
          <div className="p-6 -mt-4 relative">
            <div className="flex gap-4">
              {movie.posterPath && (
                <Image
                  src={movie.posterPath}
                  alt={movie.title}
                  width={100}
                  height={150}
                  className="rounded-lg object-cover shrink-0 shadow-lg"
                  unoptimized
                />
              )}
              <div className="flex-1 min-w-0">
                <h2 className="text-2xl font-bold leading-tight">{movie.title}</h2>
                <div className="flex flex-wrap items-center gap-3 mt-2 text-sm text-gray-400">
                  {movie.releaseYear && <span>{movie.releaseYear}</span>}
                  <span className="flex items-center gap-1">
                    <span className="text-yellow-400">★</span>
                    {Number(movie.voteAverage).toFixed(1)}
                  </span>
                  {movie.language && (
                    <span className="uppercase bg-white/10 px-2 py-0.5 rounded text-xs">
                      {movie.language}
                    </span>
                  )}
                </div>

                {/* Genres */}
                <div className="flex flex-wrap gap-1.5 mt-3">
                  {movie.genres?.map((g) => (
                    <span key={g} className="text-xs bg-brand-red/20 text-red-400 border border-brand-red/30 px-2 py-0.5 rounded-full">
                      {g}
                    </span>
                  ))}
                </div>
              </div>
            </div>

            {/* Overview */}
            {movie.overview && (
              <p className="mt-4 text-sm text-gray-300 leading-relaxed">{movie.overview}</p>
            )}

            {/* Cast */}
            {movie.cast && movie.cast.length > 0 && (
              <div className="mt-4">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-2">Cast</h3>
                <div className="flex flex-wrap gap-2">
                  {movie.cast.slice(0, 8).map((actor) => (
                    <span key={actor} className="text-xs bg-white/5 px-2 py-1 rounded text-gray-300">
                      {actor}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Rating */}
            <div className="mt-6 border-t border-brand-border pt-4">
              <h3 className="text-sm font-semibold mb-3">Rate this movie</h3>
              <StarRating movieId={movie.movieId} size="lg" />
              <p className="text-xs text-gray-500 mt-2">
                Rating updates your recommendations in real-time via WebSocket ⚡
              </p>
            </div>
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>
  );
}
