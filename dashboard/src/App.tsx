import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />} />
    </Routes>
  );
}
