"use client";

import Link from "next/link";
import { useState, useRef, useCallback } from "react";
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
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const handleSearch = useCallback((q: string) => {
    setSearchQuery(q);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (q.length < 1) { setSearchResults([]); setIsSearching(false); return; }
    setIsSearching(true);
    debounceRef.current = setTimeout(async () => {
      if (abortRef.current) abortRef.current.abort();
      abortRef.current = new AbortController();
      try {
        const { data } = await moviesApi.search(q);
        setSearchResults(data.movies.slice(0, 7));
      } catch (err: any) {
        if (err?.code !== "ERR_CANCELED") setSearchResults([]);
      } finally {
        setIsSearching(false);
      }
    }, 250);
  }, []);

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
            onKeyDown={(e) => e.key === "Escape" && (setSearchResults([]), setSearchQuery(""))}
            className="w-full bg-white/10 border border-white/20 rounded-full px-4 py-1.5 pr-8 text-sm placeholder-gray-400 focus:outline-none focus:border-brand-red transition-colors"
          />
          {/* spinner / clear button */}
          <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none">
            {isSearching ? (
              <svg className="animate-spin h-3.5 w-3.5 text-gray-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
              </svg>
            ) : searchQuery ? (
              <button
                className="pointer-events-auto text-gray-500 hover:text-white"
                onClick={() => { setSearchResults([]); setSearchQuery(""); }}
              >✕</button>
            ) : null}
          </div>
          {(searchResults.length > 0 || (isSearching && searchQuery)) && (
            <div className="absolute top-full mt-2 w-72 bg-brand-card border border-brand-border rounded-lg overflow-hidden shadow-2xl z-50">
              {isSearching && searchResults.length === 0 && (
                <div className="px-4 py-3 text-sm text-gray-400">Searching...</div>
              )}
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

