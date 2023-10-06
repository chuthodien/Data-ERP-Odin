(
    CASE 
    WHEN avatar_path IS NOT NULL 
    THEN REPLACE('<img style="width: 100%; max-height: 100%; position: absolute" src="http://project-api.nccsoft.vn/avatars/<<avatar>>" />', '<<avatar>>', avatar_path)
    ELSE '<img style="width: 100%; max-height: 100%; position: absolute" src="http://project.nccsoft.vn/assets/img/user.png" />'
    END
)