import type { NextPage } from "next";
import Link from "next/link";
import homestyles from "../../styles/Home.module.css"
export const AuthorHome = () => {
    return (
        <>
            <div className={homestyles.headerBackground}>
                <Link href="/">
                    Home
                </Link>
            </div>
            <div>Authors</div>
        </>
    )
}

export default AuthorHome;