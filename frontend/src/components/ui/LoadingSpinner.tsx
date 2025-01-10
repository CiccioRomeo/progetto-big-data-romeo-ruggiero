import styles from './loading-spinner.module.css';

export function LoadingSpinner() {
  return (
    <div className={styles.spinner}>
      <div className={styles.circle}></div>
    </div>
  );
}
