import eslint from "@eslint/js";
import parser from "@typescript-eslint/parser";
import plugin from "@typescript-eslint/eslint-plugin";
import globals from "globals";

export default [
  eslint.configs.recommended,
  {
    files: ["**/*.ts"],
    languageOptions: {
      parser,
      parserOptions: { sourceType: "module" },
      globals: globals.node,
    },
    plugins: { "@typescript-eslint": plugin },
    rules: {
      ...plugin.configs.recommended.rules,
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_" },
      ],
    },
  },
  { ignores: ["**/dist/**", "node_modules/**"] },
];
