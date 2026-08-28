import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { PortfolioPage } from "./pages/PortfolioPage";
import { PlantPage } from "./pages/PlantPage";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<PortfolioPage />} />
        <Route path="plants/:plantId" element={<PlantPage />} />
      </Route>
    </Routes>
  );
}
