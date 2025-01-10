import { Link, useLocation } from 'react-router-dom';
import { useState } from 'react';
import {
  ChartCandlestick,
  Orbit,
  Users,
  Image,
  Camera,
  Tags,
  Home,
  SearchCheck
} from 'lucide-react';
import styles from './Sidebar.module.css';

const menuItems = [
  {
    name: 'Home',
    icon: Home,
    subMenu: [],
    path: '/'
  },
  {
    name: 'Clusters',
    icon: Orbit,
    subMenu: [],
    path: '/clusters'
  },
  {
    name: 'Analisi e tendenze',
    icon: ChartCandlestick,
    subMenu: [
      { name: 'Trend fotografici', icon: Image, path: '/photos-analytics' },
      { name: 'Analisi delle camere', icon: Camera, path: '/cameras-analytics' },
      { name: 'Analisi dei tag', icon: Tags, path: '/tags-analytics' },
      { name: 'Analisi degli utenti', icon: Users, path: '/users-analytics' },
    ],
  },
  {
    name: 'Ricerca foto',
    icon: SearchCheck,
    subMenu: [],
    path: '/photo-search'
  },
];

export function Sidebar() {
  const location = useLocation();
  const [expandedMenus, setExpandedMenus] = useState<Record<string, boolean>>(
    {}
  );

  const handleMenuClick = (menuName: string) => {
    setExpandedMenus((prev) => ({
      ...prev,
      [menuName]: !prev[menuName], // Toggles the clicked menu
    }));
  };

  return (
    <div className={styles.sidebar}>
      <div className={styles.header}>
        <h2 className={styles.title}>Flickr Analytics</h2>
      </div>

      <nav className={styles.nav}>
        <ul className={styles.menuList}>
          {menuItems.map((item) => (
            <li key={item.name}>
              {/* Main Menu Button */}
              {item.subMenu.length === 0 ? (
                <Link
                  to={item.path || '/'}
                  className={`${styles.menuItem} ${
                    location.pathname === item.path ? styles.menuItemActive : ''
                  }`}
                >
                  <item.icon className={styles.menuIcon} />
                  {item.name}
                </Link>
              ) : (
                <div
                  className={`${styles.menuItem} ${
                    expandedMenus[item.name] ? styles.menuItemActive : ''
                  }`}
                  onClick={() => handleMenuClick(item.name)}
                >
                  <item.icon className={styles.menuIcon} />
                  {item.name}
                </div>
              )}

              {/* Submenu */}
              {expandedMenus[item.name] && item.subMenu.length > 0 && (
                <ul className={styles.subMenu}>
                  {item.subMenu.map((subItem) => (
                    <li key={subItem.name}>
                      <Link
                        to={subItem.path}
                        className={`${styles.menuItem} ${
                          location.pathname === subItem.path
                            ? styles.menuItemActive
                            : ''
                        }`}
                      >
                        <subItem.icon className={styles.menuIcon} />
                        {subItem.name}
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
            </li>
          ))}
        </ul>
      </nav>
    </div>
  );
}
