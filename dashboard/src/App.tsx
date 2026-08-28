import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { PortfolioPage } from "./pages/PortfolioPage";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<PortfolioPage />} />
      </Route>
    </Routes>
  );
}
