import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Toaster } from "react-hot-toast";
import { Navbar } from "@/components/Navbar";
import { AppProvider } from "@/components/AppProvider";

export const dynamic = "force-dynamic";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
  title: "CineCloud — AI-Powered Movie Recommendations",
  description:
    "Cloud-native movie recommendation engine powered by Firebase, Firestore, and hybrid collaborative + content-based filtering.",
  keywords: ["movies", "recommendations", "cloud computing", "Firebase", "serverless"],
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.variable} font-sans bg-brand-dark text-white min-h-screen`}>
        <AppProvider>
          <Navbar />
          <main className="pt-16">{children}</main>
          <Toaster position="bottom-right" />
        </AppProvider>
      </body>
    </html>
  );
}
