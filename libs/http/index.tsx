export const httpRedirect = (src: string, baseUri: string, redirectUrl: string, contentType: string): string => {
    const baseUrl = new URL(baseUri);
    const archivedFullUrl = new URL(redirectUrl);

    const updateUrl = (originalUrl: string): string => {
        const fullUri = new URL(originalUrl, baseUrl);
        archivedFullUrl.searchParams.set("uri", fullUri.toString());
        return archivedFullUrl.toString();
    };

    if (contentType.includes("text/html")) {
        const regexHtml = /(?:src|href)="([^"]*)"/gi;
        return src.replace(regexHtml, (_, attributeValue) => {
            const updatedUrl = updateUrl(attributeValue.replace(/^\.\/+/, ""));
            return `src="${updatedUrl}" href="${updatedUrl}"`;
        });
    } else if (contentType.includes("text/css")) {
        const regexCss = /url\((['"]?)([^'")]*?)\1\)/gi;
        return src.replace(regexCss, (_, quote, url) => {
            const updatedUrl = updateUrl(url);
            return `url(${quote}${updatedUrl}${quote})`;
        });
    } else {
        return src;
    }
};
