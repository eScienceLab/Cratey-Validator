import React from "react";
import { useConfig } from 'nextra-theme-docs'

export default {
  docsRepositoryBase: 'https://github.com/eScienceLab/Cratey-Validator/tree/main/docs/website',
  project: {
    link: "https://github.com/eScienceLab/Cratey-Validator",
  },
  sidebar: {
    defaultMenuCollapseLevel: 1,
  },
  logo: (
    <></>
  ),
  head() {
    const { frontMatter } = useConfig()
 
    return (
      <></>
    )
  },
  footer: false
};
