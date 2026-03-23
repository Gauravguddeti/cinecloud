import { SignIn } from "@clerk/nextjs";

export default function LoginPage() {
  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <SignIn fallbackRedirectUrl="/" signUpUrl="/register" />
    </div>
  );
}

