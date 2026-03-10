export interface Movie {
  movieId: string;
  title: string;
  overview?: string;
  posterPath?: string | null;
  backdropPath?: string | null;
  genres: string[];
  genre?: string;
  cast?: string[];
  keywords?: string[];
  voteAverage: number;
  releaseYear?: string;
  popularity?: number;
  language?: string;
}

export interface Recommendation extends Movie {
  score: number;
  reason: string;
}

export interface Rating {
  userId: string;
  movieId: string;
  rating: number;
  title: string;
  createdAt: string;
  updatedAt: string;
}

export interface User {
  userId: string;
  email: string;
  name: string;
  preferences: {
    genres: string[];
    languages: string[];
  };
  totalRatings: number;
}

export interface AuthTokens {
  accessToken: string;
  idToken: string;
  refreshToken: string;
  expiresIn: number;
}

export interface LoginResponse extends AuthTokens {
  user: User;
}

export type WsMessageType =
  | "RECOMMENDATIONS_UPDATED"
  | "ACK"
  | "PAGE_VIEW"
  | "MOVIE_CLICK";

export interface WsMessage {
  type: WsMessageType;
  recommendations?: Recommendation[];
  eventType?: string;
  timestamp?: string;
}
