import { Photo } from '@/lib/types'
import styles from './owner-card.module.css'
import { Eye, MessageCircleMore } from 'lucide-react';
import { formatNumber } from '@/components/maps/clusters/utils';

interface PhotoDisplayProps{
    photo: Photo;
    rank: number;
}

export function PhotoDisplay({ photo, rank }: PhotoDisplayProps){
    return(
        <div className={styles.photoContainer}>
            <img
                src={photo.url}
                alt={photo.url}
                className={styles.photo}
            />
            <div className={styles.photoOverlay}>
                <div className={styles.photoStats}>
                    <span className={styles.photoStat}>
                        <Eye className={styles.icon} />
                        {formatNumber(photo.views)}
                    </span>
                    <span className={styles.photoStat}>
                        <MessageCircleMore className={styles.icon} />
                        {formatNumber(photo.comments)}
                    </span>
                </div>
            </div>
            <div className={styles.rankBadge}>#{rank}</div>
        </div>
    )
}
