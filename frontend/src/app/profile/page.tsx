"use client";

import { useEffect, useState } from "react";
import { useStore } from "@/lib/store";
import { recommendationsApi, moviesApi } from "@/lib/api";
import { MovieCard } from "@/components/MovieCard";
import { StarRating } from "@/components/StarRating";
import { useRouter } from "next/navigation";
import toast from "react-hot-toast";
import type { Movie, Recommendation } from "@/lib/types";

export default function ProfilePage() {
  const { user, isAuthenticated, recommendations, ratings, loadRecommendations, setSelectedMovie } = useStore();
  const router = useRouter();
  const [refreshing, setRefreshing] = useState(false);
  const [ratedMovieDetails, setRatedMovieDetails] = useState<Movie[]>([]);
  const [loadingDetails, setLoadingDetails] = useState(false);

  useEffect(() => {
    if (!isAuthenticated) { router.push("/login"); return; }
    loadRecommendations();
  }, [isAuthenticated, router, loadRecommendations]);

  // Re-fetch movie details whenever the ratings map changes (add/remove/edit)
  useEffect(() => {
    const ids = Object.keys(ratings);
    if (ids.length === 0) { setRatedMovieDetails([]); return; }
    setLoadingDetails(true);
    Promise.all(
      ids.map((id) => moviesApi.get(id).then(({ data }) => data.movie).catch(() => null))
    ).then((results) => {
      setRatedMovieDetails(results.filter(Boolean) as Movie[]);
      setLoadingDetails(false);
    });
  }, [ratings]);

  const handleRefresh = async () => {
    if (!user) return;
    setRefreshing(true);
    try {
      await recommendationsApi.refresh(user.userId);
      await loadRecommendations();
      toast.success("Recommendations refreshed!");
    } catch {
      toast.error("Refresh failed");
    }
    setRefreshing(false);
  };

  const ratedMovies = Object.entries(ratings);

  if (!user) return null;

  return (
    <div className="max-w-6xl mx-auto px-4 md:px-8 py-8">
      {/* Profile header */}
      <div className="bg-brand-card border border-brand-border rounded-2xl p-6 mb-8 flex items-center gap-4">
        <div className="w-16 h-16 rounded-full bg-brand-red flex items-center justify-center text-2xl font-black shrink-0">
          {user.name?.[0]?.toUpperCase() || "U"}
        </div>
        <div className="flex-1">
          <h1 className="text-xl font-bold">{user.name}</h1>
          <p className="text-gray-400 text-sm">{user.email}</p>
          <p className="text-sm text-gray-500 mt-1">
            <span className="text-white font-semibold">{ratedMovies.length}</span> movies rated
          </p>
        </div>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="text-sm bg-white/10 hover:bg-white/20 border border-brand-border px-4 py-2 rounded-lg transition-colors disabled:opacity-50"
        >
          {refreshing ? "⏳ Refreshing..." : "🔄 Refresh Picks"}
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <StatCard label="Movies Rated" value={ratedMovies.length} icon="⭐" />
        <StatCard label="Recommendations" value={recommendations.length} icon="🎬" />
        <StatCard
          label="Avg Rating"
          value={ratedMovies.length
            ? (ratedMovies.reduce((sum, [, r]) => sum + r, 0) / ratedMovies.length).toFixed(1)
            : "–"}
          icon="📊"
        />
        <StatCard label="Algorithm" value="Hybrid CF+CBF" icon="🧠" />
      </div>

      {/* Your Rated Movies */}
      {ratedMovies.length > 0 && (
        <section className="mb-10">
          <h2 className="text-xl font-bold mb-4">Your Rated Movies</h2>
          {loadingDetails ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
              {ratedMovies.map(([id]) => (
                <div key={id} className="animate-pulse">
                  <div className="aspect-[2/3] bg-brand-card rounded-lg" />
                  <div className="h-3 bg-white/10 rounded mt-2 w-3/4" />
                  <div className="h-3 bg-white/10 rounded mt-1 w-1/2" />
                </div>
              ))}
            </div>
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
              {ratedMovieDetails.map((movie) => (
                <div key={movie.movieId} className="flex flex-col gap-2">
                  <div
                    className="relative cursor-pointer rounded-lg overflow-hidden group"
                    onClick={() => setSelectedMovie(movie)}
                  >
                    {movie.posterPath ? (
                      <img
                        src={movie.posterPath}
                        alt={movie.title}
                        className="w-full aspect-[2/3] object-cover group-hover:scale-105 transition-transform duration-200"
                      />
                    ) : (
                      <div className="w-full aspect-[2/3] bg-brand-card flex items-center justify-center text-gray-500 text-xs text-center px-2">
                        {movie.title}
                      </div>
                    )}
                    <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity flex items-end p-2">
                      <span className="text-xs text-white">View details</span>
                    </div>
                  </div>
                  <p className="text-xs font-medium leading-tight line-clamp-2">{movie.title}</p>
                  <StarRating movieId={movie.movieId} size="sm" />
                </div>
              ))}
            </div>
          )}
        </section>
      )}

      {/* Recommendations */}
      <section className="mb-10">
        <h2 className="text-xl font-bold mb-4">Your Recommendations</h2>
        {recommendations.length > 0 ? (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
            {recommendations.map((rec) => (
              <MovieCard key={rec.movieId} movie={rec} reason={rec.reason} />
            ))}
          </div>
        ) : (
          <div className="text-center py-12 text-gray-400 border border-dashed border-brand-border rounded-xl">
            <p>Rate some movies to see your personalised picks here!</p>
          </div>
        )}
      </section>

      {/* Cloud Architecture info */}
      <section className="bg-brand-card border border-brand-border rounded-2xl p-6">
        <h2 className="text-lg font-bold mb-4">⚙️ How Your Recommendations Are Made</h2>
        <div className="grid md:grid-cols-3 gap-4 text-sm">
          <div className="bg-black/30 rounded-xl p-4">
            <p className="font-semibold text-brand-red mb-1">① You Rate a Movie</p>
            <p className="text-gray-400">Your rating is written to Firestore and queued for async processing.</p>
          </div>
          <div className="bg-black/30 rounded-xl p-4">
            <p className="font-semibold text-yellow-400 mb-1">② Lambda Worker Triggers</p>
            <p className="text-gray-400">A background worker runs the hybrid CF+CBF algorithm on your ratings.</p>
          </div>
          <div className="bg-black/30 rounded-xl p-4">
            <p className="font-semibold text-green-400 mb-1">③ Real-Time Push</p>
            <p className="text-gray-400">New recommendations are cached in Redis and pushed via Firestore real-time — instantly.</p>
          </div>
        </div>
      </section>
    </div>
  );
}

function StatCard({ label, value, icon }: { label: string; value: string | number; icon: string }) {
  return (
    <div className="bg-brand-card border border-brand-border rounded-xl p-4">
      <div className="text-2xl mb-1">{icon}</div>
      <div className="text-xl font-bold">{value}</div>
      <div className="text-xs text-gray-400 mt-0.5">{label}</div>
    </div>
  );
}
