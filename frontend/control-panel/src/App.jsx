import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import ORBStrategy from './pages/ORBStrategy';
import DhanOrders from './pages/DhanOrders';
import SignalGeneratorSettings from './pages/settings/SignalGeneratorSettings';
import GenericJobSettings from './pages/settings/GenericJobSettings';
import TestPage from './pages/TestPage';
import { ThemeProvider } from './lib/ThemeContext';

import { Toaster } from 'sonner';

function App () {
  console.log('🚀 App component rendering');
  return (
    <ThemeProvider>
      <Toaster richColors position="top-right" />
      <Router>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Dashboard />} />
            <Route path="/test" element={<TestPage />} />
            <Route path="/orb-strategy" element={<ORBStrategy />} />
            <Route path="/dhan-orders" element={<DhanOrders />} />

            {/* Settings Routes */}
            <Route path="/settings" element={<Navigate to="/settings/signal-generator" replace />} />
            <Route path="/settings/signal-generator" element={<SignalGeneratorSettings />} />

            <Route path="/settings/intraday-feeder" element={
              <GenericJobSettings
                jobId="intraday_feeder"
                title="Intraday Feeder"
                description="Market Data Ingestion Engine"
              />
            } />
            <Route path="/settings/oracle-sync" element={
              <GenericJobSettings
                jobId="oracle_sync"
                title="Oracle Sync"
                description="Weekly Trend & Sniper Analysis"
              />
            } />
            <Route path="/settings/orb-calculator" element={
              <GenericJobSettings
                jobId="orb_calculator"
                title="ORB Calculator"
                description="Opening Range Breakout Logic"
              />
            } />
            <Route path="/settings/portfolio-manager" element={
              <GenericJobSettings
                jobId="portfolio_manager"
                title="Portfolio Manager"
                description="Trade Execution & Risk Management"
              />
            } />

            <Route path="/activity" element={<div className="p-8">Activity Log coming soon...</div>} />
          </Route>
        </Routes>
      </Router>
    </ThemeProvider>
  );
}

export default App;
