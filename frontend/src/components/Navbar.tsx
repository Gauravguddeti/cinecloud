"use client";

import Link from "next/link";
import { useState } from "react";
import { useStore } from "@/lib/store";
import { useRouter, usePathname } from "next/navigation";
import { moviesApi } from "@/lib/api";
import type { Movie } from "@/lib/types";
import clsx from "clsx";
import { useClerk } from "@clerk/nextjs";

export function Navbar() {
  const { isAuthenticated, user } = useStore();
  const { signOut } = useClerk();
  const router = useRouter();
  const pathname = usePathname();
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<Movie[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const { setSelectedMovie } = useStore();

  const handleSearch = async (q: string) => {
    setSearchQuery(q);
    if (q.length < 2) { setSearchResults([]); return; }
    setIsSearching(true);
    try {
      const { data } = await moviesApi.search(q);
      setSearchResults(data.movies.slice(0, 6));
    } catch {}
    setIsSearching(false);
  };

  const handleLogout = () => {
    signOut(() => router.push("/"));
  };

  return (
    <nav className="fixed top-0 w-full z-50 bg-gradient-to-b from-black/90 to-transparent backdrop-blur-sm border-b border-white/5">
      <div className="max-w-7xl mx-auto px-4 h-16 flex items-center gap-6">
        {/* Logo */}
        <Link href="/" className="text-brand-red font-black text-2xl tracking-tight shrink-0">
          CINE<span className="text-white">CLOUD</span>
        </Link>

        {/* Nav links */}
        <div className="hidden md:flex items-center gap-5 text-sm font-medium">
          <NavLink href="/" active={pathname === "/"}>Home</NavLink>
          <NavLink href="/browse" active={pathname === "/browse"}>Browse</NavLink>
          {isAuthenticated && (
            <NavLink href="/profile" active={pathname === "/profile"}>My List</NavLink>
          )}
        </div>

        {/* Search */}
        <div className="relative flex-1 max-w-xs ml-auto">
          <input
            type="text"
            placeholder="Search movies..."
            value={searchQuery}
            onChange={(e) => handleSearch(e.target.value)}
            className="w-full bg-white/10 border border-white/20 rounded-full px-4 py-1.5 text-sm placeholder-gray-400 focus:outline-none focus:border-brand-red transition-colors"
          />
          {searchResults.length > 0 && (
            <div className="absolute top-full mt-2 w-full bg-brand-card border border-brand-border rounded-lg overflow-hidden shadow-2xl z-50">
              {searchResults.map((movie) => (
                <button
                  key={movie.movieId}
                  onClick={() => { setSelectedMovie(movie); setSearchResults([]); setSearchQuery(""); }}
                  className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-white/5 text-left transition-colors"
                >
                  {movie.posterPath ? (
                    <img src={movie.posterPath} alt={movie.title} className="w-8 h-12 object-cover rounded" />
                  ) : (
                    <div className="w-8 h-12 bg-gray-700 rounded" />
                  )}
                  <div>
                    <p className="text-sm font-medium line-clamp-1">{movie.title}</p>
                    <p className="text-xs text-gray-400">{movie.releaseYear} · {movie.genres?.[0]}</p>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Auth actions */}
        {isAuthenticated ? (
          <div className="flex items-center gap-3 shrink-0">
            <span className="hidden md:block text-sm text-gray-300">
              Hi, {user?.name?.split(" ")[0] || "User"}
            </span>
            <button
              onClick={handleLogout}
              className="text-sm text-gray-400 hover:text-white transition-colors"
            >
              Sign out
            </button>
          </div>
        ) : (
          <div className="flex items-center gap-3 shrink-0">
            <Link href="/login" className="text-sm text-gray-300 hover:text-white transition-colors">
              Sign in
            </Link>
            <Link
              href="/register"
              className="text-sm bg-brand-red hover:bg-red-700 px-4 py-1.5 rounded-full transition-colors font-medium"
            >
              Get Started
            </Link>
          </div>
        )}
      </div>
    </nav>
  );
}

function NavLink({ href, active, children }: { href: string; active: boolean; children: React.ReactNode }) {
  return (
    <Link
      href={href}
      className={clsx("transition-colors", active ? "text-white font-semibold" : "text-gray-400 hover:text-white")}
    >
      {children}
    </Link>
  );
}

