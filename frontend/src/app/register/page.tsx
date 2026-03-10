import { SignUp } from "@clerk/nextjs";

export default function RegisterPage() {
  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <SignUp afterSignUpUrl="/" signInUrl="/login" />
    </div>
  );
}

