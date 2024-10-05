// HTMLファイルの更新時間をJSTでHTTP_headerのLast-Modifiedフォーマットで表示するJavaScriptコード

document.addEventListener('DOMContentLoaded', function() {
    // 最終更新日時を取得
    var lastModified = new Date(document.lastModified);
    
    // JSTオフセットを設定（ミリ秒単位）
    var jstOffset = 9 * 60 * 60 * 1000;
    
    // ローカル時間をJSTに変換
    var jstTime = new Date(lastModified.getTime() + jstOffset);
    
    // 曜日の配列
    var weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    
    // 月の配列
    var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    
    // フォーマットされた日時文字列を作成
    var formattedDate = weekdays[jstTime.getUTCDay()] + ", " +
                        ("0" + jstTime.getUTCDate()).slice(-2) + " " +
                        months[jstTime.getUTCMonth()] + " " +
                        jstTime.getUTCFullYear() + " " +
                        ("0" + jstTime.getUTCHours()).slice(-2) + ":" +
                        ("0" + jstTime.getUTCMinutes()).slice(-2) + ":" +
                        ("0" + jstTime.getUTCSeconds()).slice(-2) + " JST";
    
    // 表示する要素を取得
    var displayElement = document.getElementById('last-modified');
    
    // 要素が存在する場合、内容を更新
    if (displayElement) {
        displayElement.textContent = 'Last-Modified: ' + formattedDate;
    } else {
        console.log('表示要素が見つかりません。');
    }
});
