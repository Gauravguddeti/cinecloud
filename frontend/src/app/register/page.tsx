import { SignUp } from "@clerk/nextjs";

export default function RegisterPage() {
  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <SignUp fallbackRedirectUrl="/" signInUrl="/login" />
    </div>
  );
}

