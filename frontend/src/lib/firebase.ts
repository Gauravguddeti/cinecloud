/**
 * Firebase client SDK initialisation for CineCloud frontend.
 *
 * Used for:
 *   - Firestore onSnapshot → real-time recommendation updates (replaces WebSocket)
 *   - Firebase Auth → sign-in / sign-out / getIdToken()
 *
 * Config values come from Firebase Console → Project Settings → Web App.
 * Add them to frontend/.env.local (never commit that file).
 */
import { initializeApp, getApps, getApp } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore } from "firebase/firestore";

const firebaseConfig = {
  apiKey:            process.env.NEXT_PUBLIC_FIREBASE_API_KEY!,
  authDomain:        process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN!,
  projectId:         process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID!,
  storageBucket:     process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET!,
  messagingSenderId: process.env.NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID!,
  appId:             process.env.NEXT_PUBLIC_FIREBASE_APP_ID!,
};

// Prevent re-initialisation during Next.js hot reload
const app = getApps().length ? getApp() : initializeApp(firebaseConfig);

export const firebaseAuth = getAuth(app);
export const db           = getFirestore(app);
export default app;
