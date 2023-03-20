import type { NextPage } from "next";
import React from "react";
import Link from "next/link";
import styles from "../styles/Home.module.css"
import Image from 'next/image'
import authorimage from "../public/author.png"

export const Home = () => {
    return (
        <>
            <div className={styles.headerBackground}>
                <Link href="/">
                    Home
                </Link>
            </div>
            <div className={styles.mapBackground}>
                <div >
                    <p>This is for the map content</p>
                    <button type="submit">
                        <Link href="/map">
                            <a>Map</a>
                        </Link>
                    </button>
                </div>
                <div >
                    <Image src={"/world.png"} alt="" width={600} height={360} />
                </div>
            </div>
            <div className={styles.authorBackground}>
                <div >
                    <p>This is for the author content</p>
                    <button>
                        <Link href="/author">
                            <a>Author</a>
                        </Link>
                    </button>
                </div>
                <div >
                    <Image src={authorimage} alt="" width={600} height={360} />
                </div>
            </div>

        </>
    )
}


export default Home;
