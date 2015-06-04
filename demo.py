#coding:utf-8

def pagination(seq, page, page_size):
    r1, r2 = divmod(len(seq), page_size)
    page_num = r1 + int(r2 > 0)
    if page > page_num:
        page = 1

    print 'page', page
    objects = seq[(page - 1) * page_size:page * page_size]
    previous_page = page - 1 if page > 1 else None
    next_page = page + 1 if page < page_num else None

    return dict(objects=objects,
                page=page,
                page_num=page_num,
                previous_page=previous_page,
                next_page=next_page)

print pagination(range(100), 1, 10)
