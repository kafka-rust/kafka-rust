fn rule1<T,E,X: From<E>>(r: Result<T,E>) -> Result<T,X> {
    replace!(try!(r) => r?);
    unreachable!()
}