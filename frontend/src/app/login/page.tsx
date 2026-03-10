"use client";

import { useState } from "react";
import Link from "next/link";
import { useStore } from "@/lib/store";
import { useRouter } from "next/navigation";
import toast from "react-hot-toast";

export default function LoginPage() {
  const { login } = useStore();
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) return;
    setLoading(true);
    try {
      await login(email, password);
      toast.success("Welcome back!");
      router.push("/");
    } catch (err: any) {
      const msg = err?.response?.data?.error || "Login failed. Please try again.";
      toast.error(msg);
    }
    setLoading(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <div className="w-full max-w-md bg-brand-card border border-brand-border rounded-2xl p-8 shadow-2xl">
        {/* Logo */}
        <div className="text-center mb-8">
          <Link href="/" className="text-brand-red font-black text-3xl">
            CINE<span className="text-white">CLOUD</span>
          </Link>
          <p className="text-gray-400 text-sm mt-2">Sign in to get personalised recommendations</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1.5">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
              placeholder="you@example.com"
              className="w-full bg-black/40 border border-brand-border rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-brand-red transition-colors"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1.5">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="current-password"
              placeholder="••••••••"
              className="w-full bg-black/40 border border-brand-border rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-brand-red transition-colors"
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-brand-red hover:bg-red-700 disabled:opacity-60 text-white font-bold py-2.5 rounded-lg transition-colors text-sm"
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>

        <p className="text-center text-sm text-gray-400 mt-6">
          New to CineCloud?{" "}
          <Link href="/register" className="text-brand-red hover:underline font-medium">
            Create an account
          </Link>
        </p>
      </div>
    </div>
  );
}
