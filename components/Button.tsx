import React from 'react';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'danger' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
  icon?: React.ReactNode;
}

export const Button: React.FC<ButtonProps> = ({ 
  children, 
  variant = 'primary', 
  size = 'md', 
  className = '', 
  icon,
  ...props 
}) => {
  const baseStyles = "inline-flex items-center justify-center font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-1 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg active:scale-[0.98]";
  
  const variants = {
    primary: "bg-blue-600 text-white hover:bg-blue-700 hover:shadow-md hover:shadow-blue-600/20 focus:ring-blue-500 border border-transparent",
    secondary: "bg-white text-gray-700 hover:bg-gray-50 hover:text-gray-900 focus:ring-gray-200 border border-gray-200 shadow-sm hover:shadow",
    danger: "bg-red-600 text-white hover:bg-red-700 hover:shadow-md hover:shadow-red-600/20 focus:ring-red-500 border border-transparent",
    ghost: "bg-transparent text-gray-600 hover:bg-gray-100/50 hover:text-gray-900 focus:ring-gray-300",
  };

  const sizes = {
    sm: "px-3 py-1.5 text-xs",
    md: "px-4 py-2 text-sm",
    lg: "px-6 py-3 text-base",
  };

  return (
    <button 
      className={`${baseStyles} ${variants[variant]} ${sizes[size]} ${className}`} 
      {...props}
    >
      {icon && <span className="mr-2 -ml-0.5 opacity-80">{icon}</span>}
      {children}
    </button>
  );
};