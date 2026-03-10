"use client";

import { useEffect, useState, useCallback } from "react";
import { moviesApi } from "@/lib/api";
import { MovieCard } from "@/components/MovieCard";
import type { Movie } from "@/lib/types";

export default function BrowsePage() {
  const [movies, setMovies] = useState<Movie[]>([]);
  const [genres, setGenres] = useState<string[]>([]);
  const [selectedGenre, setSelectedGenre] = useState<string>("");
  const [nextToken, setNextToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);

  // Load genres
  useEffect(() => {
    moviesApi.genres().then(({ data }) => setGenres(data.genres));
  }, []);

  // Load movies when genre changes
  const loadMovies = useCallback(async (reset = false) => {
    setLoading(true);
    try {
      const token = reset ? undefined : nextToken ?? undefined;
      const { data } = await moviesApi.list({
        genre: selectedGenre || undefined,
        limit: 24,
        nextToken: token,
      });

      setMovies((prev) => reset ? data.movies : [...prev, ...data.movies]);
      setNextToken(data.nextToken ?? null);
    } catch {}
    setLoading(false);
    setInitialLoading(false);
  }, [selectedGenre, nextToken]);

  useEffect(() => {
    setNextToken(null);
    setInitialLoading(true);
    loadMovies(true);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGenre]);

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-8 py-8">
      <h1 className="text-3xl font-black mb-2">Browse Movies</h1>
      <p className="text-gray-400 text-sm mb-6">
        {movies.length > 0 ? `Showing ${movies.length} movies` : "Loading catalog..."}{" "}
        {selectedGenre && `in ${selectedGenre}`}
      </p>

      {/* Genre filter chips */}
      <div className="flex flex-wrap gap-2 mb-8">
        <GenreChip
          label="All"
          active={selectedGenre === ""}
          onClick={() => setSelectedGenre("")}
        />
        {genres.map((g) => (
          <GenreChip
            key={g}
            label={g}
            active={selectedGenre === g}
            onClick={() => setSelectedGenre(g)}
          />
        ))}
      </div>

      {/* Movie grid */}
      {initialLoading ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-4">
          {Array.from({ length: 24 }).map((_, i) => (
            <div key={i}>
              <div className="aspect-[2/3] skeleton rounded-lg" />
              <div className="h-3 skeleton rounded mt-2 w-3/4" />
              <div className="h-3 skeleton rounded mt-1 w-1/2" />
            </div>
          ))}
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-4">
            {movies.map((movie) => (
              <MovieCard key={movie.movieId} movie={movie} />
            ))}
          </div>

          {nextToken && (
            <div className="flex justify-center mt-10">
              <button
                onClick={() => loadMovies(false)}
                disabled={loading}
                className="bg-white/10 hover:bg-white/20 border border-white/20 px-8 py-2.5 rounded-full text-sm font-medium transition-colors disabled:opacity-50"
              >
                {loading ? "Loading..." : "Load More"}
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function GenreChip({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`text-xs px-3 py-1.5 rounded-full border transition-all ${
        active
          ? "bg-brand-red border-brand-red text-white font-semibold"
          : "border-brand-border text-gray-400 hover:border-white/40 hover:text-white"
      }`}
    >
      {label}
    </button>
  );
}
