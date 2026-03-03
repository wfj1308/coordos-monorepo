import React from "react";
import { createRoot } from "react-dom/client";
import JoinPage from "./JoinPage";
import "./index.css";

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <JoinPage />
  </React.StrictMode>,
);
