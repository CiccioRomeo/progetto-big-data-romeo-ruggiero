import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Sidebar } from './components/layout/Sidebar';
import styles from './App.module.css';

import { Home } from './pages/maps/Home';
import { Clusters } from './pages/maps/Clusters';

import { PhotoAnalytics } from './pages/analytics/photo/PhotoAnalytics';
import { CameraAnalytics } from './pages/analytics/cameras/CamerasAnalytics';
import { TagAnalytics } from './pages/analytics/tags/TagAnalytics';
import { PhotoSearch } from './pages/photo-search/PhotoSearch';

import { UsersAnalytics } from './pages/analytics/users/UsersAnalytics';

function App() {
  return (
    <BrowserRouter>
      <div className={styles.container}>
        <Sidebar />
        <main className={styles.main}>
          <div className={styles.content}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/clusters" element={<Clusters />} />
              <Route path="/photos-analytics" element={<PhotoAnalytics />} />
              <Route path='/cameras-analytics' element={<CameraAnalytics />} />
              <Route path="/users-analytics" element={<UsersAnalytics />} />
              <Route path="/tags-analytics" element={<TagAnalytics />} />
              <Route path="/photo-search" element={<PhotoSearch />} />
            </Routes>
          </div>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
