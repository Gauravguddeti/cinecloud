"use client";

import { useEffect, useState } from "react";
import { useStore } from "@/lib/store";
import { moviesApi } from "@/lib/api";
import { MovieCard } from "@/components/MovieCard";
import type { Movie, Recommendation } from "@/lib/types";
import Link from "next/link";
import { motion } from "framer-motion";

export default function HomePage() {
  const { isAuthenticated, recommendations, recsLoading, recsFromCache, loadRecommendations, user } = useStore();
  const [popular, setPopular] = useState<Movie[]>([]);
  const [hero, setHero] = useState<Movie | null>(null);
  const { setSelectedMovie } = useStore();

  useEffect(() => {
    moviesApi.popular(20).then(({ data }) => {
      setPopular(data.movies);
      if (data.movies.length > 0) setHero(data.movies[Math.floor(Math.random() * 5)]);
    });
  }, []);

  useEffect(() => {
    if (isAuthenticated) loadRecommendations();
  }, [isAuthenticated, loadRecommendations]);

  return (
    <div className="min-h-screen">
      {/* ── Hero Banner ───────────────────────────────── */}
      {hero && (
        <div className="relative h-[70vh] overflow-hidden">
          {hero.backdropPath || hero.posterPath ? (
            <img
              src={hero.backdropPath || hero.posterPath!}
              alt={hero.title}
              className="w-full h-full object-cover"
            />
          ) : (
            <div className="w-full h-full bg-gradient-to-r from-brand-red/30 to-purple-900/30" />
          )}
          <div className="absolute inset-0 bg-gradient-to-r from-black/90 via-black/50 to-transparent" />
          <div className="absolute inset-0 bg-gradient-to-t from-brand-dark via-transparent to-transparent" />

          <div className="absolute bottom-20 left-8 md:left-16 max-w-xl animate-fade-in">
            <div className="flex gap-2 mb-3">
              {hero.genres?.slice(0, 3).map((g) => (
                <span key={g} className="text-xs bg-brand-red/80 px-2 py-0.5 rounded-full font-medium">
                  {g}
                </span>
              ))}
            </div>
            <h1 className="text-4xl md:text-6xl font-black leading-tight">{hero.title}</h1>
            <p className="mt-3 text-sm text-gray-300 line-clamp-3 max-w-md">{hero.overview}</p>
            <div className="flex gap-3 mt-5">
              <button
                onClick={() => setSelectedMovie(hero)}
                className="bg-white text-black font-bold px-6 py-2.5 rounded-lg hover:bg-gray-200 transition-colors flex items-center gap-2"
              >
                ▶ More Info
              </button>
              <Link
                href="/browse"
                className="bg-white/20 border border-white/30 font-semibold px-6 py-2.5 rounded-lg hover:bg-white/30 transition-colors backdrop-blur-sm"
              >
                Browse All
              </Link>
            </div>
          </div>
        </div>
      )}

      <div className="px-4 md:px-8 lg:px-16 pb-20 -mt-16 relative z-10">
        {/* ── Personalized Recommendations ─────────────── */}
        {isAuthenticated && (
          <section className="mb-12">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h2 className="text-xl font-bold">
                  {user?.name ? `${user.name.split(" ")[0]}'s Picks` : "Your Recommendations"}
                </h2>
                <div className="flex items-center gap-3 mt-1">
                  <span className="text-xs text-gray-500">
                    Powered by hybrid CF + CBF algorithm
                  </span>
                  {recsFromCache ? (
                    <span className="text-xs bg-green-900/40 text-green-400 px-2 py-0.5 rounded-full border border-green-900">
                      ⚡ Cached
                    </span>
                  ) : (
                    <span className="text-xs bg-purple-900/40 text-purple-400 px-2 py-0.5 rounded-full border border-purple-900">
                      🧠 Freshly Computed
                    </span>
                  )}
                </div>
              </div>
            </div>

            {recsLoading ? (
              <SkeletonRow />
            ) : recommendations.length > 0 ? (
              <HorizontalScroll>
                {recommendations.map((rec) => (
                  <MovieCard
                    key={rec.movieId}
                    movie={rec}
                    reason={rec.reason}
                    className="w-36 shrink-0"
                  />
                ))}
              </HorizontalScroll>
            ) : (
              <EmptyRecsPrompt />
            )}
          </section>
        )}

        {/* ── Popular Right Now ────────────────────────── */}
        <section className="mb-12">
          <h2 className="text-xl font-bold mb-4">Popular Right Now</h2>
          <HorizontalScroll>
            {popular.map((movie) => (
              <MovieCard key={movie.movieId} movie={movie} className="w-36 shrink-0" />
            ))}
          </HorizontalScroll>
        </section>

        {/* ── Cloud Architecture Banner ─────────────────── */}
        {!isAuthenticated && (
          <section className="mt-12 text-center py-16 border border-brand-border rounded-2xl bg-brand-card/60 backdrop-blur-sm">
            <div className="text-5xl mb-4">☁️</div>
            <h2 className="text-3xl font-black mb-2">Cloud-Native Recommendation Engine</h2>
            <p className="text-gray-400 max-w-xl mx-auto text-sm mb-8">
              Built on AWS Lambda · DynamoDB · SQS · Cognito · CloudFront.
              Hybrid Collaborative + Content-Based Filtering, delivered in milliseconds via Upstash Redis.
            </p>
            <div className="flex flex-wrap justify-center gap-3 mb-8">
              {["Serverless", "Event-Driven", "Auto-Scaling", "WebSockets", "IaC", "CDN"].map((tag) => (
                <span key={tag} className="text-xs bg-brand-red/20 text-red-400 border border-brand-red/30 px-3 py-1.5 rounded-full">
                  {tag}
                </span>
              ))}
            </div>
            <Link
              href="/register"
              className="inline-block bg-brand-red hover:bg-red-700 text-white font-bold px-8 py-3 rounded-full transition-colors text-sm"
            >
              Get Personalized Recommendations →
            </Link>
          </section>
        )}
      </div>
    </div>
  );
}

function HorizontalScroll({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex gap-3 overflow-x-auto pb-3 scrollbar-hide -mx-2 px-2">
      {children}
    </div>
  );
}

function SkeletonRow() {
  return (
    <div className="flex gap-3 overflow-x-hidden">
      {Array.from({ length: 8 }).map((_, i) => (
        <div key={i} className="w-36 shrink-0">
          <div className="aspect-[2/3] skeleton rounded-lg" />
          <div className="h-3 skeleton rounded mt-2 w-3/4" />
          <div className="h-3 skeleton rounded mt-1 w-1/2" />
        </div>
      ))}
    </div>
  );
}

function EmptyRecsPrompt() {
  return (
    <div className="border border-dashed border-brand-border rounded-xl p-8 text-center">
      <p className="text-gray-400 text-sm">Rate some movies to get personalised recommendations!</p>
      <Link href="/browse" className="inline-block mt-3 text-brand-red text-sm hover:underline">
        Browse movies →
      </Link>
    </div>
  );
}
