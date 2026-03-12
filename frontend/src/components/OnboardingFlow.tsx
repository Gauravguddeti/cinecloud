"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import Image from "next/image";
import { motion, AnimatePresence } from "framer-motion";
import { moviesApi } from "@/lib/api";
import { useStore } from "@/lib/store";
import type { Movie } from "@/lib/types";

const MIN_RATINGS = 5;

interface RatedEntry {
  movie: Movie;
  rating: number;
}

export function OnboardingFlow({ reason }: { reason?: string }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Movie[]>([]);
  const [searching, setSearching] = useState(false);
  const [rated, setRated] = useState<RatedEntry[]>([]);
  const [hovered, setHovered] = useState<{ id: string; star: number } | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const { submitRating, loadRecommendations } = useStore();

  // Autofocus search on mount
  useEffect(() => { inputRef.current?.focus(); }, []);

  const handleSearch = useCallback((q: string) => {
    setQuery(q);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (q.length < 1) { setResults([]); setDropdownOpen(false); return; }
    setSearching(true);
    debounceRef.current = setTimeout(async () => {
      if (abortRef.current) abortRef.current.abort();
      abortRef.current = new AbortController();
      try {
        const { data } = await moviesApi.search(q, abortRef.current.signal);
        const ratedIds = new Set(rated.map((r) => r.movie.movieId));
        setResults(data.movies.filter((m) => !ratedIds.has(m.movieId)).slice(0, 7));
        setDropdownOpen(true);
      } catch (err: any) {
        if (err?.code !== "ERR_CANCELED") setResults([]);
      } finally {
        setSearching(false);
      }
    }, 250);
  }, [rated]);

  const addRating = (movie: Movie, rating: number) => {
    setRated((prev) => {
      const without = prev.filter((r) => r.movie.movieId !== movie.movieId);
      return [...without, { movie, rating }];
    });
    setQuery("");
    setResults([]);
    setDropdownOpen(false);
    inputRef.current?.focus();
  };

  const removeRated = (movieId: string) => {
    setRated((prev) => prev.filter((r) => r.movie.movieId !== movieId));
  };

  const changeRating = (movieId: string, rating: number) => {
    setRated((prev) =>
      prev.map((r) => (r.movie.movieId === movieId ? { ...r, rating } : r))
    );
  };

  const handleDone = async () => {
    if (rated.length < MIN_RATINGS || submitting) return;
    setSubmitting(true);
    try {
      // submitRating in store already calls ratingsApi.submit + updates local state
      await Promise.all(
        rated.map(({ movie, rating }) => submitRating(movie.movieId, rating))
      );
      await loadRecommendations();
    } catch {
      setSubmitting(false);
    }
  };

  const remaining = MIN_RATINGS - rated.length;
  const ready = rated.length >= MIN_RATINGS;

  return (
    <div className="min-h-screen flex items-center justify-center px-4 py-16"
      style={{ background: "radial-gradient(ellipse at 50% 0%, #1a0a0f 0%, #0a0a0f 60%)" }}>
      {/* Subtle film-grain overlay */}
      <div className="fixed inset-0 pointer-events-none opacity-[0.03]"
        style={{ backgroundImage: "url(\"data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E\")" }} />

      <motion.div
        className="w-full max-w-xl"
        initial={{ opacity: 0, y: 32 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
      >
        {/* Header */}
        <motion.div
          className="text-center mb-8"
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <div className="text-4xl mb-3">🎬</div>
          <h1 className="text-3xl font-black text-white mb-2">
            {reason === "ratings_dropped"
              ? "We lost your taste profile"
              : "Tell us what you love"}
          </h1>
          <p className="text-gray-400 text-sm leading-relaxed">
            {reason === "ratings_dropped"
              ? `Rate ${remaining} more movie${remaining !== 1 ? "s" : ""} to restore your personalised recommendations.`
              : "Rate 5 movies so we can personalise your feed.\nThe more you rate, the better it gets."}
          </p>
        </motion.div>

        {/* Progress bar */}
        <motion.div
          className="flex gap-2 mb-6 justify-center"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2 }}
        >
          {Array.from({ length: MIN_RATINGS }).map((_, i) => (
            <div
              key={i}
              className="h-1.5 rounded-full transition-all duration-500"
              style={{
                width: 40,
                background: i < rated.length ? "#f5c518" : "rgba(255,255,255,0.1)",
                boxShadow: i < rated.length ? "0 0 8px #f5c51880" : "none",
              }}
            />
          ))}
        </motion.div>

        {/* Glass card */}
        <motion.div
          className="rounded-2xl p-6"
          style={{
            background: "rgba(255,255,255,0.04)",
            backdropFilter: "blur(12px)",
            border: "1px solid rgba(255,255,255,0.08)",
          }}
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          {/* Search */}
          <div className="relative mb-4">
            <div className="flex items-center gap-2 bg-white/8 border border-white/12 rounded-xl px-4 py-3 focus-within:border-[#f5c518]/60 transition-colors"
              style={{ background: "rgba(255,255,255,0.06)" }}>
              <span className="text-gray-500 text-sm">🔍</span>
              <input
                ref={inputRef}
                type="text"
                placeholder="Search for a movie…"
                value={query}
                onChange={(e) => handleSearch(e.target.value)}
                onFocus={() => results.length > 0 && setDropdownOpen(true)}
                onKeyDown={(e) => e.key === "Escape" && (setDropdownOpen(false), setQuery(""))}
                className="flex-1 bg-transparent text-white text-sm placeholder-gray-500 outline-none"
              />
              {searching && (
                <svg className="animate-spin h-4 w-4 text-gray-400 shrink-0" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
                </svg>
              )}
            </div>

            {/* Dropdown */}
            <AnimatePresence>
              {dropdownOpen && results.length > 0 && (
                <motion.div
                  className="absolute top-full mt-2 w-full rounded-xl overflow-hidden z-50 shadow-2xl"
                  style={{
                    background: "rgba(18,18,24,0.98)",
                    border: "1px solid rgba(255,255,255,0.1)",
                    backdropFilter: "blur(16px)",
                  }}
                  initial={{ opacity: 0, y: -8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -8 }}
                  transition={{ duration: 0.15 }}
                >
                  {results.map((movie) => (
                    <SearchResultRow
                      key={movie.movieId}
                      movie={movie}
                      onRate={(rating) => addRating(movie, rating)}
                    />
                  ))}
                </motion.div>
              )}
            </AnimatePresence>
          </div>

          {/* Rated cards */}
          <AnimatePresence>
            {rated.length > 0 && (
              <motion.div
                className="flex gap-3 overflow-x-auto pb-2 scrollbar-hide mb-4"
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 1, height: "auto" }}
                exit={{ opacity: 0, height: 0 }}
              >
                {rated.map(({ movie, rating }, idx) => (
                  <motion.div
                    key={movie.movieId}
                    className="shrink-0 relative"
                    style={{ width: 80 }}
                    initial={{ opacity: 0, scale: 0.8, y: 8 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.8 }}
                    transition={{ delay: idx * 0.04 }}
                  >
                    <div className="relative rounded-lg overflow-hidden" style={{ height: 120 }}>
                      {movie.posterPath ? (
                        <Image src={movie.posterPath} alt={movie.title} fill className="object-cover" unoptimized />
                      ) : (
                        <div className="w-full h-full bg-gray-800 flex items-center justify-center text-2xl">🎬</div>
                      )}
                      <button
                        onClick={() => removeRated(movie.movieId)}
                        className="absolute top-1 right-1 w-5 h-5 rounded-full bg-black/70 flex items-center justify-center text-xs text-gray-300 hover:bg-red-600 hover:text-white transition-colors"
                      >✕</button>
                    </div>
                    <p className="text-[10px] text-gray-400 mt-1 line-clamp-1 text-center">{movie.title}</p>
                    <InlineStars
                      value={rating}
                      onChange={(r) => changeRating(movie.movieId, r)}
                    />
                  </motion.div>
                ))}
              </motion.div>
            )}
          </AnimatePresence>

          {/* Hint */}
          <p className="text-xs text-gray-600 text-center mb-4">
            💡 Try rating movies from different genres for better results
          </p>

          {/* CTA */}
          <motion.button
            onClick={handleDone}
            disabled={!ready || submitting}
            className="w-full py-3.5 rounded-xl font-bold text-sm transition-all duration-300"
            style={{
              background: ready ? "linear-gradient(135deg, #f5c518, #e6b800)" : "rgba(255,255,255,0.06)",
              color: ready ? "#0a0a0f" : "#555",
              cursor: ready && !submitting ? "pointer" : "not-allowed",
              boxShadow: ready ? "0 0 24px rgba(245,197,24,0.35)" : "none",
            }}
            animate={ready ? { boxShadow: ["0 0 16px rgba(245,197,24,0.3)", "0 0 28px rgba(245,197,24,0.55)", "0 0 16px rgba(245,197,24,0.3)"] } : {}}
            transition={{ duration: 2, repeat: Infinity }}
          >
            {submitting
              ? "Building your feed…"
              : ready
              ? "Show My Recommendations →"
              : `Rate ${remaining} more movie${remaining !== 1 ? "s" : ""} to continue`}
          </motion.button>
        </motion.div>

        <p className="text-center text-xs text-gray-700 mt-4">
          {rated.length} / {MIN_RATINGS} rated
        </p>
      </motion.div>
    </div>
  );
}

// ── Sub-components ────────────────────────────────────────────

function SearchResultRow({ movie, onRate }: { movie: Movie; onRate: (r: number) => void }) {
  const [hoveredStar, setHoveredStar] = useState(0);
  const [selected, setSelected] = useState(0);

  const pick = (r: number) => {
    setSelected(r);
    onRate(r);
  };

  return (
    <div className="flex items-center gap-3 px-3 py-2.5 hover:bg-white/5 transition-colors">
      <div className="shrink-0 rounded overflow-hidden" style={{ width: 32, height: 48 }}>
        {movie.posterPath ? (
          <Image src={movie.posterPath} alt={movie.title} width={32} height={48} className="object-cover w-full h-full" unoptimized />
        ) : (
          <div className="w-full h-full bg-gray-800 flex items-center justify-center text-sm">🎬</div>
        )}
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm text-white font-medium line-clamp-1">{movie.title}</p>
        <p className="text-xs text-gray-500">{movie.releaseYear}{movie.genres?.[0] ? ` · ${movie.genres[0]}` : ""}</p>
      </div>
      <div className="flex gap-0.5 shrink-0">
        {[1, 2, 3, 4, 5].map((s) => (
          <button
            key={s}
            onMouseEnter={() => setHoveredStar(s)}
            onMouseLeave={() => setHoveredStar(0)}
            onClick={() => pick(s)}
            className="text-lg leading-none transition-transform hover:scale-110"
            style={{ color: s <= (hoveredStar || selected) ? "#f5c518" : "#333" }}
          >★</button>
        ))}
      </div>
    </div>
  );
}

function InlineStars({ value, onChange }: { value: number; onChange: (r: number) => void }) {
  const [hov, setHov] = useState(0);
  return (
    <div className="flex justify-center gap-0.5 mt-0.5">
      {[1, 2, 3, 4, 5].map((s) => (
        <button
          key={s}
          onMouseEnter={() => setHov(s)}
          onMouseLeave={() => setHov(0)}
          onClick={() => onChange(s)}
          className="text-xs leading-none"
          style={{ color: s <= (hov || value) ? "#f5c518" : "#333" }}
        >★</button>
      ))}
    </div>
  );
}
